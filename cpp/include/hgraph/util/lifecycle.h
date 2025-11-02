//
// Created by Howard Henson on 05/05/2024.
//

#ifndef LIFECYCLE_H
#define LIFECYCLE_H

#include<hgraph/hgraph_base.h>

namespace hgraph {
    struct ComponentLifeCycle;

    void HGRAPH_EXPORT initialise_component(ComponentLifeCycle &component);

    void HGRAPH_EXPORT start_component(ComponentLifeCycle &component);

    void HGRAPH_EXPORT stop_component(ComponentLifeCycle &component);

    void HGRAPH_EXPORT dispose_component(ComponentLifeCycle &component);

    struct TransitionGuard;

    /**
     * This will intialise the component in the constructor and dispose in the destructor.
     * This is the closest equivalent to the Python Context Manager.
     * The destructor allows exceptions to propagate, matching Python's finally block behavior.
     */
    struct InitialiseDisposeContext {
        InitialiseDisposeContext(ComponentLifeCycle &component);

        ~InitialiseDisposeContext() noexcept; // Destructors must not throw; exceptions are reported earlier
    private:
        ComponentLifeCycle &_component;
    };

    /**
     * This will start the component in the constructor and stop in the destructor.
     * This is the closest equivalent to the Python Context Manager.
     * The destructor allows exceptions to propagate, matching Python's finally block behavior.
     */
    struct StartStopContext {
        StartStopContext(ComponentLifeCycle &component);

        ~StartStopContext() noexcept; // Never throws - call stop() explicitly if you need exception propagation
    private:
        ComponentLifeCycle &_component;
    };


    /**
     * The Life-cycle and associated method calls are as follows:
     *
     * * The component is constructed using the __init__, additional properties may be set after this
     *   in order to deal with reverse dependencies, etc.
     *
     * * The component will have initialise called, in a graph context this call is in topological sort order.
     *
     * * The start method is called prior to normal operation of the code, this can perform actions such as schedule node
     *   evaluation, and should also delegate the call to any components managed by this component.
     *
     * * The stop method is called once normal operation of the code is expected to cease. This can be used to stop threads
     *   or perform any state clean-up required.
     *
     * * The dispose method is called once the component is no longer required and in a graph context will be called in
     *   reverse topological sort order.
     *
     * NOTE: The start and stop life-cycle methods can be called numerous times during the life-time of the component.
     *       The code should ensure that it is able to start again cleanly after stop has be called. Stop is not dispose,
     *       full clean-up is called only on dispose.
     *
     * Since any life-cycle controlled component is likely to reasonably heavy weight, it seems reasonable to make them
     * ref-counting as well. *** May change my mind about this, but for now seems reasonable ***
     */
    struct HGRAPH_EXPORT ComponentLifeCycle : nb::intrusive_base {
        virtual ~ComponentLifeCycle() = default;

        /**
         * The componented is started (true) or stopped (false).
         * By default, this is stopped.
         */
        [[nodiscard]] bool is_started() const;

        /**
         * The component is in the process of starting.
         */
        [[nodiscard]] bool is_starting() const;

        /**
         * The process is in the process of stopping.
         */
        [[nodiscard]] bool is_stopping() const;

        static void register_with_nanobind(nb::module_ &m);

    protected:
        /**
         * Called once the component has been constructed and prepared.
         * Use this life-cycle call to prepare cached data, etc. This is called once after construction and never again.
         * If this component creates life-cycle managed components then it should delegate this call to them at this point
         * in time, however if the component is provided with life-cycle managed components, then it is NOT the
         * responsibility of this component to send intialise or dispose calls.
         */
        virtual void initialise() = 0;

        /**
         * Perform any actions required to initialise the component such as establishing threads, or scheduling
         * initial tasks, etc. This method must ensure the is_started property becomes True once this has been called.
         * It is the responsibility of the component to delegate the start life-cycle call to ALL contained life-cycle
         * managed components.
         */
        virtual void start() = 0;

        /**
         * Perform any actions required to halt the activities of the component, this may entail activities such as stopping
         * threads, resetting state, etc. This method must ensure the is_started property becomes False once this method
         * has been called.
         * It is the responsibility of the component to delegate the stop life-cycle call to ALL contained life-cycle
         * managed components.
         */
        virtual void stop() = 0;

        /**
         * Use this life-cycle call to clean up any resources held. This is called once only at the end of the components
         * life-cycle and its expected the component will be released after this call completes. This ensures
         * That any resources that are referenced are cleaned up in a timely fashion.
         * When called in the graph context, the order of initialise and dispose are also ensured.
         * It is the responsibility of this component to delegate this call to any life-cycle managed components
         * that were constructed by this component ONLY, components set on the component are NOT to be delegated to.
         */
        virtual void dispose() = 0;

    private:
        bool _started{false};
        bool _transitioning{false};

        friend TransitionGuard;

        friend void initialise_component(ComponentLifeCycle &component);

        friend void start_component(ComponentLifeCycle &component);

        friend void stop_component(ComponentLifeCycle &component);

        friend void dispose_component(ComponentLifeCycle &component);
    };
}

#endif //LIFECYCLE_H